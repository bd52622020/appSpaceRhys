class human:
    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name
        
    def __str__(self):
        print("This human's name is " + self.first_name + " " + self.last_name + ". ", end='')
    
    
class student(human):
    def __init__(self, first_name, last_name, nick):
        self.skype_nick = nick
        super(student, self).__init__(first_name, last_name)
        
    def __str__(self):
        super().__str__()
        print("They are a student and their skype nickname is " + self.skype_nick + ".")
        
class researcher(human):
    def __init__(self, first_name, last_name, classes):
        self.classes = classes
        super(researcher, self).__init__(first_name, last_name)
        
    def __str__(self):
        super().__str__()
        print("They are a researcher and they teach ", end='')
        print(*self.classes, sep=', ', end='')
        print(".")
       
def main():
    Aleksander = researcher("Aleksander", "Fabijian",["class1","class2"])
    Frida = student("Frida", "Jacobson","Frida96")
    Aleksander.__str__()
    Frida.__str__()

if __name__ == "__main__":
    main()