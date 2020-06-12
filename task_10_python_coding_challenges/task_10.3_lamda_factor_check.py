#!/usr/bin/env python

def main(list1):
    list2 = list(filter(lambda x: ((x%9 == 0) | (x%13 == 0)) & (x != 0),list1))
    print("original: " + str(list1))
    print("divisible by 9 or 13: " + str(list2))
    return(list2)

if __name__ == "__main__":
    list1 = [0, 0, 1, 2, 3, 4, 4, 5, 6, 6, 6, 7, 8, 9, 4, 4, 13, 26] 
    main(list1)