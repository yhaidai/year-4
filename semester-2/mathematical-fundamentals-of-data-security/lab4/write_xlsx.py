import re

import xlsxwriter

if __name__ == '__main__':
    p = 5879
    message = 'the preparations for the conference were carried out with the utmost care'
    workbook = xlsxwriter.Workbook('shamir.xlsx')
    worksheet1 = workbook.add_worksheet('A')
    worksheet2 = workbook.add_worksheet('B')

    with open('out.txt') as file:
        content = file.read()
        match = re.findall(
            r'(\d+)\((\w| )\), cA = (\d+), dA = (\d+), cB = (\d+), dB = (\d+)',
            content
        )
        print(match)
        for i, (m, ch, cA, dA, cB, dB) in enumerate(match):
            m, cA, dA, cB, dB = map(int, (m, cA, dA, cB, dB))
            print(m, ch, cA, dA, cB, dB)
            x1 = pow(m, cA, p)
            x2 = pow(x1, cB, p)
            x3 = pow(x2, dA, p)
            x4 = pow(x3, dB, p)

            for col, value in enumerate((ch, m, cA, dA, x1, x2, x3)):
                worksheet1.write(i, col, value)

            for col, value in enumerate((cB, dB, x1, x2, x3, x4, m, ch)):
                worksheet2.write(i, col, value)

    workbook.close()
