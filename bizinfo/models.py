import sqlalchemy as sql
import sqlalchemy.orm as orm


class Base(orm.DeclarativeBase):
    pass


class SupportProgram(Base):
    __tablename__ = "support_programs"

    id: orm.Mapped[int] = orm.mapped_column(primary_key=True, autoincrement=True)
    pblanc_id: orm.Mapped[str] = orm.mapped_column(sql.String, unique=True, comment="공고ID")
    inst: orm.Mapped[str] = orm.mapped_column(sql.String, comment="소관부처·지자체")
    type: orm.Mapped[str] = orm.mapped_column(sql.String, comment="지원분야")
    title: orm.Mapped[str] = orm.mapped_column(sql.String, comment="지원사업명")
    url: orm.Mapped[str] = orm.mapped_column(sql.String, comment="url주소")
    hashtags: orm.Mapped[str] = orm.mapped_column(sql.String, default="", comment="해시태그")
    created_at: orm.Mapped[str] = orm.mapped_column(sql.String, comment="등록일자")


def migrate(engine: sql.engine.Engine):
    Base.metadata.create_all(engine)


def upsert(session: orm.Session, items: list[dict]):
    """pblancId 기준 upsert. 있으면 update, 없으면 insert."""
    from sqlalchemy.dialects.postgresql import insert

    if not items:
        return

    stmt = insert(SupportProgram).values(items)
    stmt = stmt.on_conflict_do_update(
        index_elements=["pblanc_id"],
        set_={
            "inst": stmt.excluded.inst,
            "type": stmt.excluded.type,
            "title": stmt.excluded.title,
            "url": stmt.excluded.url,
            "hashtags": stmt.excluded.hashtags,
            "created_at": stmt.excluded.created_at,
        },
    )
    session.execute(stmt)
    session.commit()
