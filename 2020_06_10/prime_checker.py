#!/usr/bin/env python


def main(upper):
  
    for num in range(1,upper + 1):  
        if num > 1:  
            for i in range(2,num):  
                if (num % i) == 0:  
                    break  
            else:  
                print(num)  
    
if __name__ == "__main__":
    x = input("Please enter a number: ")    
    main(int(x))