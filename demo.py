import sys
from src.forest.exception import CustomException

def test():
    try:
        x = 1/0
    except Exception as e:
        raise CustomException(e,sys)

if __name__ == '__main__':
    try:
        test()
    except Exception as e:
        print(e)