from flask import Flask
import os
from flaskhadoop.db import get_db
from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort
from .storage import HadoopStorage
from werkzeug.utils import secure_filename
import requests
from requests_gssapi import HTTPSPNEGOAuth


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass


    # index page

    def get_delegation_token():
        r = requests.get(f"http://{g.storage.hadoop_host}:{g.storage.hadoop_port}/webhdfs/v1/?op=GETDELEGATIONTOKEN", auth=HTTPSPNEGOAuth())
        token = r.json()['Token']['urlString']
        return token

    @app.route('/')
    def index():
        g.storage = HadoopStorage()
        db = get_db()
        posts = db.execute(
            'SELECT id, title, filename, created'
            ' FROM post'
            ' ORDER BY created DESC'
        ).fetchall()

        token = get_delegation_token()

        return render_template('index.html', posts=posts, storage=g.storage, auth=f'delegation={token}')


    @app.route('/create', methods=('GET', 'POST'))
    def create():
        if request.method == 'POST':
            g.storage = HadoopStorage()
            title = request.form['title']
            file = request.files['file']

            filename = g.storage.save(secure_filename(file.filename), file)

            error = None
            if not title:
                error = 'Title is required.'

            elif not file:
                error = 'File is required.'

            if error is not None:
                flash(error)
            else:
                db = get_db()
                db.execute(
                    'INSERT INTO post (title, filename)'
                    ' VALUES (?, ?)',
                    (title, filename)
                )
                db.commit()
                return redirect(url_for('index'))

        return render_template('create.html')
    

    def get_post(id, check_author=True):
        post = get_db().execute(
            'SELECT id, title, filename'
            ' FROM post'
            ' WHERE id = ?',
            (id,)
        ).fetchone()

        if post is None:
            abort(404, f"Post id {id} doesn't exist.")

        return post



    @app.route('/<int:id>/update', methods=('GET',))
    def update(id):
        post = get_post(id)
        g.storage = HadoopStorage()
        token = get_delegation_token()

        return render_template('update.html', post=post, storage=g.storage, auth=f'delegation={token}')


    @app.route('/<int:id>/delete', methods=('POST',))
    def delete(id):
        post = get_post(id)

        try:
            g.storage = HadoopStorage()
            g.storage.delete(post['filename'])

        except:
            flash('Could not delete from HDFS')
            return redirect(url_for('index'))
            

        db = get_db()
        db.execute('DELETE FROM post WHERE id = ?', (id,))
        db.commit()

        return redirect(url_for('index'))


    from . import db
    db.init_app(app)

    return app