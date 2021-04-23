# @ app.route('/product/<name>')
# def get_product(name):
#     return "The product is " + str(name)


# @ app.route('/create/<first_name>/<last_name>')
# def create(first_name=None, last_name=None):
#     return 'Hello ' + first_name + ',' + last_name


# @ app.route('/login', methods=['POST', 'GET'])
# def login():
#     if request.method == 'POST':
#         user = request.form['name']
#         return redirect(url_for('hello_world'))
#     else:
#         user = request.args.get('name')
#         return "hello"


# @ app.route('/hello')
# def hello_world():
#     return "Hello world"

# @ app.route("/mail")
# def send():
#     msg = Message('Hello', sender=os.getenv('EMAIL'),
#                   recipients=[os.getenv('EMAIL')])
#     msg.body = "This is the email body"
#     mail.send(msg)
#     return "Sent"


# @ app.route('/db', methods=['POST'])
# def create_record():
#     # return request.form
#     record = request.form
#     # return record['name']
#     user = Users(name=record['name'],
#                  email=record['email'])
#     user.save()
#     return jsonify(user)

# @ app.route('/db', methods=['GET'])
# def query_records():
#     email = request.args.get('email')
#     user = Users.objects(email=email).first()
#     if not user:
#         return jsonify({'error': 'data not found'})
#     else:
#         return jsonify(user)
