#!/usr/bin/env python

def main(n):
    n = n - 1
    a = [1]

    for k in range(max(n ,0)):

        a.append(int(a[k]*(n-k)/(k+1)))

    print(max(a))
    
    
        
    
    
    
if __name__ == "__main__":
    n = input("Enter number of rows: ")
    main(int(n))