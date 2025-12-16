from flask import Flask, render_template
app = Flask(__name__)

@app.route('/')
def index():
    return "Welcome to my Flask Web Application!"

@app.route('/about')
def about():
    return render_template('about.html')

#poooooooo

if __name__ == '__main__':
    app.run(debug= True)