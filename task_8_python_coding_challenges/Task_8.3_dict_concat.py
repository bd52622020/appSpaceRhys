#!/usr/bin/env python

def main(a,b,c):
    d = {**a, **b,**c} #Requires python 3.5+
    print(d)
    
if __name__ == "__main__":
    a={1:10, 2:20} 
    b={3:30, 4:40} 
    c={5:50,6:60} 
    main(a,b,c)