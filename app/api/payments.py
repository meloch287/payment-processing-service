import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Request, Response, status

from app.api.deps import SessionDep
from app.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentRead
from app.serializers import to_accepted, to_read
from app.services import IdempotencyConflictError, create_payment, get_payment

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
)

IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[\x21-\x7e]+$")
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={status.HTTP_409_CONFLICT: {"model": ErrorResponse}},
)
async def create(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: SessionDep,
    request: Request,
    response: Response,
) -> PaymentAccepted:
    try:
        result = await create_payment(session, body, idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from exc
    response.headers["Location"] = f"{router.prefix}/{result.payment.id}"
    if result.created:
        request.app.state.wake_outbox_relay()
    else:
        response.headers["Idempotent-Replayed"] = "true"
    return to_accepted(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
)
async def retrieve(payment_id: uuid.UUID, session: SessionDep) -> PaymentRead:
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return to_read(payment)
