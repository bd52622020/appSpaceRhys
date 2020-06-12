#!/usr/bin/env python
import random

def main(team="chelsea",wins=0,draws=0,losses=0):
    taunt = {1:"wololo", 2:"waaaaa"}
    good = {1:"you rule", 2:"awesome"}
    bad = {1:"you suck", 2:"terrible"}
    points = (wins * 3) + losses
    print(str(team) + " got " + str(points) + " points.")
    if points > 20:
        print(random.choice(list(taunt.values())) + " " + random.choice(list(good.values())))
    else:
        print(random.choice(list(taunt.values())) + " " + random.choice(list(bad.values()))) 
    
if __name__ == "__main__":
    team = input("Please enter your team: ")
    wins = input("Please enter your team's wins: ")
    draws = input("Please enter your team's draws: ")    
    main(team,int(wins),int(draws))