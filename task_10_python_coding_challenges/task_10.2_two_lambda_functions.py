#/usr/bin/env python

def main(a):
    plus_fifteen = lambda x: x+15   
    times_two = lambda x,y: x*y 
    b = plus_fifteen(a)
    print(times_two(a,b))
    
    
if __name__ == "__main__":
    a = int(input("Please enter a number: "))
    main(a)