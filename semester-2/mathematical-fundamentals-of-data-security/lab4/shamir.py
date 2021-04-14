from random import randint

import xlsxwriter


def c_d(p):
    while True:
        c = randint(2, 1000)
        for d in range(1, 100_000):
            if (c * d) % (p - 1) == 1:
                return c, d


if __name__ == '__main__':
    p = 5807
    message = 'unfortunately you did not answer my question and did not take into account my remark'

    workbook = xlsxwriter.Workbook('shamir_.xlsx')
    cell_format = workbook.add_format()
    cell_format.set_align('center')
    cell_format.set_align('vcenter')

    worksheet1 = workbook.add_worksheet('A')
    worksheet1.add_table(f'A1:G{len(message) + 1}', {
        'columns': [
            {'header': 'Символ'},
            {'header': 'm'},
            {'header': 'cA'},
            {'header': 'dA'},
            {'header': 'x1'},
            {'header': 'x2'},
            {'header': 'x3'}
        ],
    })
    worksheet2 = workbook.add_worksheet('B')
    worksheet2.add_table(f'A1:H{len(message) + 1}', {
        'columns': [
            {'header': 'cB'},
            {'header': 'dB'},
            {'header': 'x1'},
            {'header': 'x2'},
            {'header': 'x3'},
            {'header': 'x4'},
            {'header': 'm'},
            {'header': 'Символ'}
        ]
    })

    for i, ch in enumerate(message, start=1):
        m = ord(ch) - 87 if ch != ' ' else 36
        cA, dA = c_d(p)
        cB, dB = c_d(p)
        x1 = pow(m, cA, p)
        x2 = pow(x1, cB, p)
        x3 = pow(x2, dA, p)
        x4 = pow(x3, dB, p)
        print(f'm{i} = {m}({ch}), cA = {cA}, dA = {dA}, cB = {cB}, dB = {dB}')
        print(f'x1 = {m} ** {cA} mod {p} = {x1}')
        print(f'x2 = {x1} ** {cB} mod {p} = {x2}')
        print(f'x3 = {x2} ** {dA} mod {p} = {x3}')
        print(f'x4 = {x3} ** {dB} mod {p} = {x4}', end='\n\n')

        for col, value in enumerate((ch, m, cA, dA, x1, x2, x3)):
            worksheet1.write(i, col, value, cell_format)

        for col, value in enumerate((cB, dB, x1, x2, x3, x4, m, ch)):
            worksheet2.write(i, col, value, cell_format)

    workbook.close()
