#!/usr/bin/env python


def main(limit):
    
    primes = []
    for n in range(1,limit + 1,1):  
        if n > 1:
            if 2 not in primes:
                primes.append(2)
            for i in range(2,n):  
                if n % i == 0 or n in primes:  
                    break  
                else:  
                    primes.append(n)
    print(primes)
    
if __name__ == "__main__":
    x = input("Please enter a number: ")    
    main(int(x))