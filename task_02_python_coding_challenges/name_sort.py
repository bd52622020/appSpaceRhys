#!/usr/bin/env python
from collections import OrderedDict 

def main(dict1):
    print("Dict sorted by surname:")
    dict2 = (OrderedDict(sorted(dict1.items())))
    print(dict2)

if __name__ == "__main__":
    test_dict = {"John":"Doe","Amy":"Brooks","Ranjit":"Sambi","Julia":"Cortez","Robert":"Locke"}
    main(test_dict)