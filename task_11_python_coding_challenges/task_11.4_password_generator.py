import random
import string

class generator:
    def __init__(self, pwlength = 16):
        self.output = ""
        self.pwlength = pwlength

    def main(self):
        choice_list = string.ascii_letters + string.digits + string.punctuation
        for _ in range(self.pwlength):
            self.output = self.output + random.choice(choice_list)
        return(self.output)
        
if __name__ == "__main__":
    passw = generator(32)
    print(passw.main())