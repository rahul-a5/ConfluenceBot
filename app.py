from flask import Flask, render_template, request
from model import get_answer

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/process', methods=['POST'])
def process():
    text = request.form['text']
    ans = get_answer(text)
    processed_text = f"Your answer: {ans}"
    return render_template('home.html', processed_text=processed_text)

if __name__ == '__main__':
    app.run(debug=True)
