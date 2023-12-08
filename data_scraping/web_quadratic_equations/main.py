from math import sqrt
from io import BytesIO
from base64 import b64encode

from numpy import linspace, round
import matplotlib
import matplotlib.pyplot as plt
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates


matplotlib.use('Agg')

app = FastAPI()
templates = Jinja2Templates(directory="templates")


def solve_quadratic(a, b, c):
    discriminant = b**2 - 4 * a * c
    if discriminant < 0:
        return []
    elif discriminant == 0:
        root = -b / (2 * a)
        return [root]
    else:
        sqrt_discriminant = sqrt(discriminant)
        root1 = (-b + sqrt_discriminant) / (2 * a)
        root2 = (-b - sqrt_discriminant) / (2 * a)
        return [root1, root2]


def get_plot(a, b, c, roots):
    if len(roots) == 2:
        distance = roots[1] - roots[0]
        x1 = roots[0] - distance * .3
        x2 = roots[1] + distance * .3
    elif len(roots) == 1:
        x1 = roots[0] - 10
        x2 = roots[0] + 10
    else:
        x1 = -10
        x2 = 10
    x = linspace(x1, x2, 400)
    y = a * x**2 + b * x + c

    b_sign = "+" if b > 0 else "-"
    c_sign = "+" if c > 0 else "-"
    fig = plt.figure()
    plt.plot(x, y)
    plt.xlabel('x')
    plt.ylabel('y')
    plt.axhline(0, color='black', linewidth=0.5)
    plt.axvline(0, color='black', linewidth=0.5)
    plt.grid(True, linestyle='--', linewidth=0.5)
    plt.title(f'Parabola of {a}x^2 {b_sign} {abs(b)}x {c_sign} {abs(c)}')
    if roots:
        plt.scatter(roots, [0] * len(roots), color='red', label='Roots')
        plt.legend()

    pngImage = BytesIO()
    fig.savefig(pngImage)
    pngImageBase64String = b64encode(pngImage.getvalue()).decode('ascii')

    return pngImageBase64String


@app.get("/")
def main(request: Request):
    return templates.TemplateResponse("main.html", {"request": request})


@app.get("/solve")
def solve(a: int, b: int, c: int):
    roots = solve_quadratic(a, b, c)
    return {"roots": roots}


@app.get("/plot")
def plot(request: Request, a: int, b: int, c: int):
    roots = solve_quadratic(a, b, c)
    picture = get_plot(a, b, c, roots)
    if not roots:
        show_roots = 'No roots'
    else:
        show_roots = [round(root, 4) for root in roots]
    return templates.TemplateResponse("main.html", {"request": request, "roots": show_roots, "plot": picture})
