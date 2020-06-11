#!/usr/bin/env python
from math import ceil

def main(speed):
    if speed <= 70:
        print("OK.")
    else:
        points = ceil((speed-70)/5)
        print("Points: " + str(points))
        if points > 12:
            print("License suspended.")
    
if __name__ == "__main__":
    speed = input("Please enter the driver's speed: ")
    main(int(speed))