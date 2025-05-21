from app import create_app
from flask import redirect, url_for

app = create_app()

# Add a route to redirect root to the Tailwind UI by default
@app.route('/')
def index_redirect():
    return redirect('/?use_tailwind=true')

if __name__ == '__main__':
    print("Running with Tailwind UI enabled by default")
    app.run(host='0.0.0.0', port=8081, debug=True) 