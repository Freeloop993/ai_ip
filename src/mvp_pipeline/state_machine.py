ALLOWED_TRANSITIONS = {
    "collected": {"analyzing", "failed"},
    "analyzing": {"evaluated", "failed"},
    "evaluated": {"producing", "failed"},
    "producing": {"pending_review", "failed"},
    "pending_review": {"publishing", "pending_rework"},
    "pending_rework": {"producing", "failed"},
    "publishing": {"published", "failed"},
    "published": {"tracking"},
    "tracking": set(),
    "failed": {"analyzing", "producing", "pending_review"},
}


def validate_transition(from_status: str, to_status: str) -> None:
    allowed = ALLOWED_TRANSITIONS.get(from_status, set())
    if to_status not in allowed:
        raise ValueError(f"invalid transition: {from_status} -> {to_status}")
