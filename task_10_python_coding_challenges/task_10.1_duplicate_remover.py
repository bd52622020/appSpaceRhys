#!/usr/bin/env python

def main(list1):
    list2 = list(dict.fromkeys(list1))
    print("orginal: " + str(list1))
    print("without duplicates: " + str(list2))
    
if __name__ == "__main__":
    list1 = [0, 0, 1, 2, 3, 4, 4, 5, 6, 6, 6, 7, 8, 9, 4, 4]
    main(list1)