#!/usr/bin/env python
from datetime import date

def main(dict1):
    str1 = input("please enter a name: ").lower()
    try:
        print(str1 + "'s birthday is " + str(dict1[str1]))
    except:
        print("name not found.")
    
if __name__ == "__main__":
    dict1={'jim':date(1956,1,1),
           'sarah':date(1992,1,1),
           'mike':date(1968,1,1),
           'ranjit':date(1987,1,1),
           'tomoko':date(1994,1,1),
           'ron':date(1995,1,1),
           'ali':date(1990,1,1),
           'isabella':date(1970,1,1),
           'dan':date(1978,1,1)      
           } 
    main(dict1)