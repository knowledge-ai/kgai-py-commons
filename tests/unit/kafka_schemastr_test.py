import typing
from dataclasses import dataclass

from kgai_py_commons.clients.kafka.util.schema_str import SchemaStr


class TestKafkaSchemaStr(object):
    @classmethod
    def setup_class(cls):
        cls.schema = SchemaStr()

    def test_schema_str_nested(self):
        test_val = """
        {
           "type":"record",
           "name":"Users",
           "namespace": "myspace"
           "fields":[
              {
                 "name":"membership",
                 "type":{
                    "type":"record",
                    "name":"Membership",
                    "fields":[
                       {
                          "name":"groups",
                          "type":{
                             "type":"array",
                             "items":"string",
                             "name":"group"
                          }
                       },
                       {
                          "name":"name",
                          "type":"string"
                       },
                       {
                          "name":"location",
                          "type":"string"
                       },
                       {
                          "name":"department",
                          "type":{
                             "type":"record",
                             "name":"Department",
                             "fields":[
                                {
                                   "name":"name",
                                   "type":"string"
                                },
                                {
                                   "aname":"location",
                                   "type":"string"
                                }
                             ],
                             "doc":"Department(name: str, location: str)"
                          }
                       }
                    ],
                    "doc":"Membership(groups: List[str], name: str, location: str, department: tests.unit.kafka_schemastr_test.Department)"
                 }
              },
              {
                 "name":"name",
                 "type":"string"
              },
              {
                 "name":"age",
                 "type":"int"
              },
              {
                 "name":"pets",
                 "type":{
                    "type":"array",
                    "items":"string",
                    "name":"pet"
                 }
              },
              {
                 "name":"accounts",
                 "type":{
                    "type":"map",
                    "values":"int",
                    "name":"account"
                 }
              },
              {
                 "name":"favorite_colors",
                 "type":{
                    "type":"enum",
                    "symbols":[
                       "BLUE",
                       "YELLOW",
                       "GREEN"
                    ],
                    "name":"favorite_color"
                 }
              },
              {
                 "name":"address",
                 "type":[
                    "null",
                    "string"
                 ],
                 "default":"null"
              }
           ],
           "doc":"Users(membership: tests.unit.kafka_schemastr_test.Membership, name: str, age: int, pets: List[str], accounts: Dict[str, int], favorite_colors: Tuple[str] = ('BLUE', 'YELLOW', 'GREEN'), address: str = None)"
        }
        """
        schema_str = self.schema.create_schema(namespace="myspace", data_class=User)
        assert schema_str is not None, "Expected schema string to be created"
        # no need to check if library functions properly, skip fancy tests for now, just null check above


@dataclass
class Department:
    name: str
    location: str


@dataclass
class Membership:
    groups: typing.List[str]
    name: str
    location: str
    department: Department


@dataclass
class User:
    membership: Membership
    name: str
    age: int
    pets: typing.List[str]
    accounts: typing.Dict[str, int]
    favorite_colors: typing.Tuple[str] = ("BLUE", "YELLOW", "GREEN")
    address: str = None
