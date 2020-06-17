#!/usr/bin/env python
import pandas as pd
    
#Import data
titanic_df = pd.read_csv("titanic.csv")

#Q1.1
print("Question 1.1:\n")
print(titanic_df.info())

#Q1.2
print("\n\nQuestion 1.2:\n")
print("Column labels: " + str(titanic_df.columns.values.tolist()))
print("Shape: " + str(list(titanic_df.shape)))
print("dtypes: " + str(list(titanic_df.dtypes)))

#Q1.3
print("\n\nQuestion 1.3:\n")
titanic_pivot_df = titanic_df[['Age', 'Pclass', 'Survived','Sex']].pivot_table(index='Sex')
print(titanic_pivot_df)

#Q1.4
print("\n\nQuestion 1.4:\n")
print("Passengers under 10: " + str(titanic_df[titanic_df["Age"]<10]['Name']))
print("\nPassengers under 20 and over 10: " + str(titanic_df[(titanic_df["Age"]<20) & (titanic_df["Age"]>10)]['Name']))
print("\nPassengers under 30 and over 20: " + str(titanic_df[(titanic_df["Age"]<30) & (titanic_df["Age"]>20)]['Name']))
print("\nPassengers over 30: " + str(titanic_df[(titanic_df["Age"]>30) & (titanic_df["Age"]>30)]['Name']))

#Q1.5
print("\n\nQuestion 1.5:\n")
titanic_child = titanic_df.where(titanic_df['Age'] < 18).pivot_table( index='Sex',aggfunc='count')['Name']
titanic_adult = titanic_df.where(titanic_df['Age'] >= 18).pivot_table( index='Sex',aggfunc='count')['Name']
print(pd.concat([titanic_child,titanic_adult], keys=['child', 'adult']))
