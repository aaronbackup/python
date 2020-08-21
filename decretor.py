import sys
def new_derotor(f):
    def wrapper_func():
        print("before wrapper")
        f()
        print("after wrapper")

    return wrapper_func

@new_derotor
def func():
    print("decretor test")

if __name__ == "__main__":
    func()



