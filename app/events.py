import uuid

from pydantic import BaseModel, ConfigDict

PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: uuid.UUID
    event_type: str = PAYMENT_CREATED
    payment_id: uuid.UUID
