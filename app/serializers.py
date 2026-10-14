from app.models import Payment
from app.schemas import PaymentAccepted, PaymentRead, WebhookDelivery


def to_accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


def to_read(payment: Payment) -> PaymentRead:
    return PaymentRead(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        status=payment.status,
        failure_reason=payment.failure_reason,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        webhook=WebhookDelivery(
            attempts=payment.webhook_attempts,
            delivered_at=payment.webhook_delivered_at,
            last_error=payment.webhook_last_error,
        ),
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
