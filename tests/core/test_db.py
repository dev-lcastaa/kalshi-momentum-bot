from decimal import Decimal

from sqlalchemy import select

from kmb.db import init_db, make_engine, session_factory, session_scope
from kmb.db.models import Job, RiskLimit, User


def test_init_db_and_roundtrip(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path / 'kmb.db'}")
    init_db(engine)
    init_db(engine)  # idempotent
    sf = session_factory(engine)
    with session_scope(sf) as s:
        u = User(username="lcastaa", password_hash="x", role="admin")
        s.add(u)
        s.flush()
        s.add(Job(user_id=u.id, kind="sim", spec={"windows": ["w1"]}))
    with session_scope(sf) as s:
        job = s.scalar(select(Job))
        assert job.status == "queued" and job.spec == {"windows": ["w1"]}
        limits = s.scalars(select(RiskLimit)).all()
        assert len(limits) == 1 and limits[0].max_order_count == Decimal("10")
