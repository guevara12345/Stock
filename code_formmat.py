import re


class Formatter:
    def code2capita(self, code):
        m = re.match('(\S{2})\.(\d{6})', code)
        return (m.group(1)+m.group(2)).upper()

    def code2nopoint(self, code):
        m = re.match('(\S{2})\.(\d{6})', code)
        return m.group(1)+m.group(2)


code_formatter = Formatter()

if __name__ == '__main__':
    print(code_formatter.code2capita('sh.600346'))
    print(code_formatter.code2nopoint('sh.600346'))
