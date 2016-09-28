# -*- coding: utf-8 -*-

import json, shutil
import datetime, time

from gluon.tools import fetch

from archiver import DBSyncer, rsync


def dbsync(waits=0, force=False):
    """ Fetch the remote archives and store it into db """

    source = myconf.take("source.source")

    logger.debug("=== FETCHALL ===: Starting to fetching all data from: %s" % source)

    if waits>0:
        time.sleep(waits)

    last_update = db.archive.last_update.max()
    res = db(db.archive.id>0).select(
        db.archive.archname,
        last_update,
        groupby = db.archive.archname,
    )

    now = datetime.datetime.now()
    def checkrepo(archname):
        if force:
            return True
        fres = res.find(lambda r: r.archive.archname==archname, limitby=(0,1,)).first()
        if fres is None:
            # se nessun download presente
            return True
        else:
            # se ultima modifica è troppo vecchia
            delta = (now-fres[last_update])
            return delta.total_seconds()>=appconf[archname]["period"]

    # downloadable archives
    archives = dict([(k,v) for k,v in myconf.iteritems() \
        if k.startswith('arch_') and myconf.take("%s.ignore" % k, cast=boolean)!=True and checkrepo(k)
    ])

    fetched = {}
    if len(archives)>0:
        with DBSyncer(table=db.archive, **myconf.take(source)) as oo:
            for k,nfo in archives.iteritems():
                current.logger.debug("Considering archive: %s" % k)
                success = oo.fetch(k, **nfo)
                if success:
                    fetched[k] = nfo
            #oo.rsync()
    return {
        "fetched_archives": fetched,
        "len": len(fetched),
    }

class sync(object):
    """ """

    @staticmethod
    def _go():
        """
        Returns True/False depending on whether I got an updated catalog or not
        """

        out = {}

        res = dbsync(force=current.development)
        logger.info("Fetched %(len)s archives:\n\t%(fetched_archives)s" % {k: json.dumps(v, indent=4) for k,v in res.iteritems()})
        logger.info("=== End of fetching source! ===")

        mainarch = ("arch_cat", "arch_pri", "arch_avl", "arch_apt",)

        # Elaborazione congiunta degli archivi di catalogo, prezzi e disponibilità
        # per preparazione file dei prodotti (completo ed eventualmente quello parziale)
        if any([i in res["fetched_archives"] for i in mainarch]):
            if archive.build_updates(updated=res["fetched_archives"].keys(), clean=not current.development)>0:
                out.update(archive.compile_csv())

        # Elaborazioni singole (solo per archivi di immagini)
        otherarch =  [a for a in res["fetched_archives"] if not a in mainarch]
        if len(otherarch)>0:
            for row in db(db.archive.archname.belongs(otherarch)).select():
                res = archive.retrieve(row)
                current.logger.info("File from %s archive copied to tmp destination." % db.archive)
                out[row.archive] = res

        return out

    @classmethod
    def go(cls):
        tmp_content = os.listdir(myconf.take("dest.tmp_path", cast=lpath))
        if tmp_content:
            logger.warning("Temporary folder found not empty!")
            shutil.rmtree(appconf.dest.tmp_path)
            logger.warning("Temporary folder cleaned.")
#         if len(cls._go())>0:
#             rsync()
