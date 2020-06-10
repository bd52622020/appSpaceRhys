#!/usr/bin/env python

def main(isbn):
    
    isbn = list(map(int,list(str(isbn).replace('-',''))))
    if len(isbn) == 10:
        x=0
        for idx, i in enumerate(isbn):
            x+=((10-idx)*i)
        if not x% 11:
            print("valid.") 
        else:
            print("not valid.")
    else:
        print("not valid.")   
        
if __name__ == "__main__":
    isbn = input("Please enter a ISBN: ")    
    main(isbn)