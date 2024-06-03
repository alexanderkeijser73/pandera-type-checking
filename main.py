import dataclasses
from typing import cast

import pandas as pd
import pandera as pa
import pydantic

from pandera.typing import Series, DataFrame

class Schema(pa.DataFrameModel):
    name: Series[str]
    age: Series[int]

class Schema2(Schema):
    banana: Series[int]

class Schema3(Schema2):
    apple: Series[int]


bla = DataFrame[Schema2](pd.DataFrame.from_dict({
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "banana": [1, 2, 3],
}))

# TODO: can't refactor-> change name of variable with Pycharm :(
print(bla.age)
print(bla.banana)
print(bla['apples'])
print(bla.apples)

class Schema4(Schema):
   apples: str
def func2(df: DataFrame[Schema]) -> DataFrame[Schema4]:
    df.apples = df.name + df.age
    return cast(DataFrame[Schema4], df)

def func(df: DataFrame[Schema2]) -> DataFrame[Schema3]:
    return df.pipe(DataFrame[Schema3])

func(bla)


# @dataclasses.dataclass
# class Person:
#     name: str
#     agel: int
#
# def get_person():
#     return Person(name="Alice", agel=25)
#
# dataclass_person = get_person()
# # this does complain
# print(dataclass_person.size)
# # TODO: can refactor-> change name of variable with Pycharm :)
# print(dataclass_person.agel)
#
#
# ##########################################################
# # pydantic
# ##########################################################
#
# class Person2(pydantic.BaseModel):
#     name: str
#     agel: int
#
# person2 = Person2(name="Alice", age=25)
# # TODO: can refactor-> change name of variable with Pycharm :)
# print(person2.agel)
# # this does complain (not Pycharm, but Mypy does)
# print(person2.size)
#
#
# ##########################################################
# # sqlalchemy
# ##########################################################
# from sqlalchemy import Column, Integer, String, select
# from sqlalchemy.orm import declarative_base
#
# # "Base" is a class that is created dynamically from the
# # declarative_base() function
# Base = declarative_base()
#
#
# class User(Base):
#     __tablename__ = "user"
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#
#
# # "some_user" is an instance of the User class, which
# # accepts "id" and "name" kwargs based on the mapping
# some_user = User(id=5, name="user")
#
# # it has an attribute called .name that's a string
# print(f"Username: {some_user.name}")
# bla = some_user.banana
#
# # a select() construct makes use of SQL expressions derived from the
# # User class itself
# select_stmt = select(User).where(User.id.in_([3, 4, 5])).where(User.name.contains("s"))
#
#
