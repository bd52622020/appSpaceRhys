#!/usr/bin/env python

def main(a,b,c):
    a*=a
    b*=b
    c*=c
   
    if max(a,b,c) == (a+b+c-max(a,b,c)):
        print("Data produces a right sided Triangle.")
    else:
        print("Data does not produce a right sided Triangle.")        
    
if __name__ == "__main__":
    a = int(input("enter first triangle side length: "))
    b = int(input("enter second triangle side length: "))
    c = int(input("enter third triangle side length: "))        
    main(a,b,c)