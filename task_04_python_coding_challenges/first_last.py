#!/usr/bin/env python

def main(list1):
    if list1[0] == list1.pop():
        return(True)
    else:
        return(False)
        
if __name__ == "__main__":
    list1 = [2,3,4,5,6,7,8,3]  
    print(main(list1))