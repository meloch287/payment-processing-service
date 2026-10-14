import uuid
from datetime import timedelta

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import ChargeRequest, PaymentGateway
from app.consumer.webhook import WebhookDeliveryError, WebhookSender, build_payload
from app.models import Payment, PaymentStatus


class PaymentNotFoundError(Exception):
    pass


class PaymentInProgressError(Exception):
    pass


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook_sender: WebhookSender,
        lease_seconds: float = 30.0,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook_sender = webhook_sender
        self._lease = timedelta(seconds=lease_seconds)

    async def process(self, payment_id: uuid.UUID) -> Payment:
        payment, webhook_claimed = await self._settle(payment_id)
        if payment.webhook_delivered_at is None:
            payment = await self._notify(payment, webhook_claimed)
        return payment

    async def _settle(self, payment_id: uuid.UUID) -> tuple[Payment, bool]:
        payment = await self._claim(payment_id)
        if payment.status is not PaymentStatus.PENDING:
            return payment, False
        result = await self._gateway.charge(
            ChargeRequest(payment_id=payment.id, amount=payment.amount, currency=payment.currency)
        )
        async with self._session_factory() as session, session.begin():
            settled = (
                await session.scalars(
                    update(Payment)
                    .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                    .values(
                        status=result.status,
                        failure_reason=result.failure_reason,
                        processed_at=func.now(),
                        processing_lease_until=None,
                        webhook_lease_until=func.now() + self._lease,
                    )
                    .returning(Payment)
                    .execution_options(populate_existing=True)
                )
            ).one_or_none()
            if settled is None:
                return await self._load(session, payment_id), False
        return settled, True

    async def _claim(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session, session.begin():
            claimed = (
                await session.scalars(
                    update(Payment)
                    .where(
                        Payment.id == payment_id,
                        Payment.status == PaymentStatus.PENDING,
                        or_(
                            Payment.processing_lease_until.is_(None),
                            Payment.processing_lease_until < func.now(),
                        ),
                    )
                    .values(processing_lease_until=func.now() + self._lease)
                    .returning(Payment)
                    .execution_options(populate_existing=True)
                )
            ).one_or_none()
            if claimed is not None:
                return claimed
            payment = await self._load(session, payment_id)
        if payment.status is PaymentStatus.PENDING:
            raise PaymentInProgressError(str(payment_id))
        return payment

    @staticmethod
    async def _load(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
        payment = (
            await session.scalars(
                select(Payment)
                .where(Payment.id == payment_id)
                .execution_options(populate_existing=True)
            )
        ).one_or_none()
        if payment is None:
            raise PaymentNotFoundError(str(payment_id))
        return payment

    async def _notify(self, payment: Payment, claimed_already: bool) -> Payment:
        claimed = payment if claimed_already else await self._claim_webhook(payment.id)
        if claimed is None:
            async with self._session_factory() as session:
                current = await self._load(session, payment.id)
            if current.webhook_delivered_at is not None:
                return current
            raise PaymentInProgressError(str(payment.id))
        try:
            await self._webhook_sender.send(claimed.webhook_url, build_payload(claimed))
        except WebhookDeliveryError as exc:
            await self._record_delivery(payment.id, error=str(exc))
            raise
        return await self._record_delivery(payment.id, error=None)

    async def _claim_webhook(self, payment_id: uuid.UUID) -> Payment | None:
        async with self._session_factory() as session, session.begin():
            return (
                await session.scalars(
                    update(Payment)
                    .where(
                        Payment.id == payment_id,
                        Payment.webhook_delivered_at.is_(None),
                        or_(
                            Payment.webhook_lease_until.is_(None),
                            Payment.webhook_lease_until < func.now(),
                        ),
                    )
                    .values(webhook_lease_until=func.now() + self._lease)
                    .returning(Payment)
                    .execution_options(populate_existing=True)
                )
            ).one_or_none()

    async def _record_delivery(self, payment_id: uuid.UUID, error: str | None) -> Payment:
        values: dict[str, object] = {
            "webhook_attempts": Payment.webhook_attempts + 1,
            "webhook_last_error": error,
            "webhook_lease_until": None,
        }
        if error is None:
            values["webhook_delivered_at"] = func.now()
        async with self._session_factory() as session, session.begin():
            return (
                await session.scalars(
                    update(Payment)
                    .where(Payment.id == payment_id)
                    .values(**values)
                    .returning(Payment)
                    .execution_options(populate_existing=True)
                )
            ).one()
