from itertools import product
from math import floor

from numpy import polydiv
from scipy.interpolate import lagrange


def divisors(x):
    return {i for i in range(-abs(x), abs(x) + 1) if i != 0 and x % i == 0}


def func(x, polynomial):
    return sum([x ** i * c for i, c in enumerate(polynomial[::-1])])


def factorize(polynomial):
    n = len(polynomial) - 1
    m = floor(n / 2) + 1
    d = [divisors(func(x, polynomial)) for x in range(m)]
    p = list(product(*d))
    print([func(x, polynomial) for x in range(m)])
    for _ in map(print, d):
        pass
    print(p)
    print({
        str(lagrange(list(range(m)), y)).split('\n')[1] for y in p
        if len(lagrange(list(range(m)), y).coefficients) > 1
           and all(map(lambda x: x == int(x), lagrange(list(range(m)), y)))
    })

    for y in p:
        f = lagrange(list(range(m)), y)
        if len(f.coefficients) > 1 and all(
                map(lambda x: x == int(x), f.coefficients)):
            g = polydiv(polynomial, f.coefficients)
            if not any(g[1]) and all(map(lambda x: x == int(x), g[0])):
                print(str(f)[2:])
                print(g, '\n')
                return f, factorize([int(x) for x in g[0]])

    return polynomial


if __name__ == '__main__':
    polynomial = [1, 1, -21, -45]
    print(factorize(polynomial))
