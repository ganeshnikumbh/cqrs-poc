# -*- coding: utf-8 -*-
# DBへの接続設定
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base,as_declarative,declared_attr
from sqlalchemy.orm import sessionmaker, scoped_session


# 接続したいDBの基本情報を設定
user_name = "user"
password = "password"
host = "db"  # docker-composeで定義したMySQLのサービス名
database_name = "sample_db"

DATABASE = 'mysql://%s:%s@%s/%s?charset=utf8' % (
    user_name,
    password,
    host,
    database_name,
)

# DBとの接続
ENGINE = create_engine(
    DATABASE,
    encoding="utf-8",
    echo=True
)

# # Sessionの作成
# session = scoped_session(
#     # ORM実行時の設定。自動コミットするか、自動反映するか
#     sessionmaker(
#         autocommit=False,
#         autoflush=False,
#         bind=ENGINE
#     )
# )


# Sessionの作成
session = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=ENGINE
    )

@as_declarative()
class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    

# # modelで使用する
# Base = declarative_base()
# # DB接続用のセッションクラス、インスタンスが作成されると接続する
# Base.query = session.query_property()
