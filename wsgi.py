from app import app

server = app.server

if __name__ == '__main__':
    server.run(debug=False, host='127.0.0.1', port=8002)
