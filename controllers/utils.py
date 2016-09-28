# -*- coding: utf-8 -*-

def cleandb():
    db.archive.truncate("CASCADE")
    db.commit()
    return dict()
