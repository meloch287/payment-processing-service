import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.events import PaymentCreatedEvent
from app.messaging import NEW_PAYMENTS_QUEUE
from app.models import OutboxEvent, Payment, PaymentStatus
from app.schemas import PaymentCreate


class IdempotencyConflictError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CreateResult:
    payment: Payment
    created: bool


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> CreateResult:
    request_hash = data.fingerprint()
    payment_id = uuid.uuid4()
    statement = (
        insert(Payment)
        .values(
            id=payment_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            meta=data.metadata,
            status=PaymentStatus.PENDING,
            webhook_url=str(data.webhook_url),
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    async with session.begin():
        payment = (await session.scalars(statement)).one_or_none()
        if payment is None:
            existing = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if existing is None or existing.request_hash != request_hash:
                raise IdempotencyConflictError
            return CreateResult(payment=existing, created=False)
        event = PaymentCreatedEvent(event_id=uuid.uuid4(), payment_id=payment.id)
        session.add(
            OutboxEvent(
                id=event.event_id,
                aggregate_id=payment.id,
                event_type=event.event_type,
                routing_key=NEW_PAYMENTS_QUEUE,
                payload=event.model_dump(mode="json"),
            )
        )
    return CreateResult(payment=payment, created=True)


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
