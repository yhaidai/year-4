def gcd(a, b):
    if b == 0:
        return a, 1, 0

    d, y, x = gcd(b, a % b)
    y -= a // b * x
    print(d, x, y)
    return d, x, y


if __name__ == '__main__':
    print(gcd(3520495, 564828))
