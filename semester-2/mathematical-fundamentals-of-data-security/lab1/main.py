import os
import time


def gcd(a, b, c, forward_filename, backward_filename, u=None, v=None, u_prev=None, v_prev=None, u_prev_prev=None,
        v_prev_prev=None):
    a, b = max(a, b), min(a, b)

    with open(forward_filename, 'a+') as f:
        f.write(f'{a} = {a // b} * {b} + {a % b}\n')

    if a % b == 0:
        d = b
        s = c * u // b
        t = c * v // b
        return b, (u, v), (s, t)
    else:
        m = -(a // b)
        if u is None and v is None:
            u = 1
            v = m
        else:
            if u_prev is None and v_prev is None:
                u_prev = u
                v_prev = v
                u = u * m
                v = v * m + 1
            else:
                u_prev_prev, v_prev_prev = u_prev, v_prev
                u_prev, v_prev = u, v
                u = u * m + u_prev_prev
                v = v * m + v_prev_prev

        u_sign, v_sign = '', '-'
        if v > 0:
            v_sign = '+'
        if u < 0:
            u_sign = '-'
        with open(backward_filename, 'a+') as f:
            f.write(f'{a % b} = {a} - {-m} * {b} = {u_sign}{abs(u)}a {v_sign} {abs(v)}b\n')

        return gcd(b, a % b, c, forward_filename, backward_filename, u, v, u_prev, v_prev)


if __name__ == '__main__':
    forward_filename = 'forward.txt'
    backward_filename = 'backward.txt'
    try:
        os.remove(forward_filename)
        os.remove(backward_filename)
    except FileNotFoundError:
        pass
    a1, b1, c1 = 288, 88, 40
    a2, b2, c2 = 13273706, 2264466, 14934
    a3, b3, c3 = 8338378103479608, 1939482877349796, 112662
    t = time.time()
    for i in range(100):
        d = gcd(a1, b1, c1, forward_filename, backward_filename)
        # print(d)
    print((time.time() - t) / 100)
