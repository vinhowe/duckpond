from typing import Optional

from pydantic import BaseModel
from datetime import datetime


class Member(BaseModel):
    id: str
    phone: str
    created: datetime
    muted: Optional[bool] = False

    @classmethod
    def from_db(cls, data):
        return cls(
            id=data["id"],
            phone=data["phoneNumber"],
            created=datetime.fromisoformat(data["created"]),
        )
