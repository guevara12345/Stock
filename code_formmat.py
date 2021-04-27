import re


class Formatter:
    def code2capita(self, code):
        m = re.match(r'(\S{2})\.(\d{4,6})', code)
        if m:
            return (m.group(1)+m.group(2)).upper()
        else:
            # 非a股code
            return code

    def code2nopoint(self, code):
        m = re.match(r'(\S{2})\.(\d{4,6})', code)
        if m:
            return m.group(1)+m.group(2)
        else:
            # 非a股code
            return code

    def code2code_without_char(self, code):
        m = re.match(r'(\S{2})\.(\d{4,6})', code)
        if m:
            return m.group(2)
        else:
            # 非a股code
            return code
    def islevel2code(self,level2code):
        m = re.match(r'S(\d{4})', level2code)
        if m:
            return True
        else: 
            return False

code_formatter = Formatter()

if __name__ == '__main__':
    print(code_formatter.code2capita('sh.600346'))
    print(code_formatter.code2nopoint('sh.600346'))
