#!/usr/bin/env python

def main(list1):
    list2 = list(map(lambda x: x*x,list1))
    print(list2)
    return(list2)
    
if __name__ == "__main__":
    list1=[10, 20, 30, 40, 50, 60] 
    main(list1)