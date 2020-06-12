#!/usr/bin/env python


def main(x,y):
    long_edge = max(x,y)
    short_edge = min(x,y)
    third_max = long_edge + short_edge
    third_min = long_edge - short_edge
    
    print(str(third_min) + " < third edge length < " + str(third_max))
 
    
if __name__ == "__main__":
    x = input("Please enter the first edge length of the triangle: ")
    y = input("Please enter the second edge length of the triangle: ")    
    main(int(x),int(y))