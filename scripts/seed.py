import sys
import uuid
from datetime import datetime, timedelta, timezone

import httpx

API = "http://localhost:8000"
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
USERS = ["alice", "bob", "carol"]


def main(n: int) -> None:
    now = datetime.now(timezone.utc)
    with httpx.Client(base_url=API, timeout=10) as client:
        for i in range(n):
            tx = {
                "id": str(uuid.uuid4()),
                "user_id": USERS[i % len(USERS)],
                "amount": round(10 + i * 1.5, 2),
                "currency": CURRENCIES[i % len(CURRENCIES)],
                "timestamp": (now - timedelta(minutes=i)).isoformat(),
            }
            r = client.post("/transactions", json=tx)
            r.raise_for_status()
        print(f"sent {n} transactions")
        print("summary(alice):", client.get("/users/alice/summary").json())
        print("history:", client.get("/users/alice/transactions", params={"page_size": 5}).json())


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 20)
