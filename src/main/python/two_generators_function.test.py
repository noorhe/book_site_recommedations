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

file = open('test.jl', 'w')
file.write("фывавыпып")
file.close()

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
