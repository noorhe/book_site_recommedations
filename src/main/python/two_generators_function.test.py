import json
from dataclasses import dataclass

def gen1():
    i = 0
    for i in range(3):
        yield i

def gen2():
    j = 0
    for j in ["a", "b", "c"]:
        yield j

def gen3():
    yield 4

def two_generators(d):
    print("before")
    i = 0
    for i in range(3):
        print("before yield i")
        yield i
    j = 0
    for j in ["a", "b", "c"]:
        print("before yield j")
        yield j
    print("after")

class UserIdPagePair:
    def __init__(self, userId, page=1):
        self.__type__ = "UserIdPagePair"
        self.userId = userId
        self.page = page

    def __hash__(self):
        return hash((self.userId, self.page))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.userId == other.userId and self.page == other.page
        
    def __str__(self):
        return f"UserIdPagePair: (userId: {self.userId}, page: {self.page})"
        
class LogEntry:
    a = 0
    __type__ = "ty"
    def __init__(self, op, type, data):
        self.op = op
        self.type = type
        self.data = data
        
class PersonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UserIdPagePair):
            return obj.__dict__
        if isinstance(obj, LogEntry):
            return obj.__dict__
        if isinstance(obj, StringPair):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)
        
@dataclass
class StringPair:
    str1: str
    str2: str
    __type__: str = "StringPair"
        

pair = StringPair("string1", "string2")    
uid1 = UserIdPagePair("asd", 1)
uid2 = UserIdPagePair("fds", 1)
s = {uid1, uid2}
obj = LogEntry("pull", "user_user", uid1)
line = json.dumps(("str1", "str2"), cls = PersonEncoder, ensure_ascii=False)
print(line)
result = ""
for item in s:
    start = "" if result == "" else f"{result}, "
    result = f"{start}{item}"
result = "{" + result + "}"
print(result)


# for i in two_generators(True):
#     print(i)
#     print("---")
# print("\n")
# for i in two_generators(False):
#     print(i)
# print("\n")
# two_generators(True)
# for i in two_generators(True):
#     print(i)
# for i in gen3():
#     print(i)
