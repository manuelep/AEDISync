# -*- coding: utf-8 -*-

from archiver import get_dest_parts, get_dest_path, SheetReader, stock, keep_fs_clean
import xlrd, csv, zipfile

from datadiff.tools import assert_equal

def not_equal(a, b):
    try:
        assert_equal(a, b)
    except AssertionError:
        return True
    else:
        return False

class archive(object):
    """ Tools to manage archive data """

    @staticmethod
    def _copy(stream, filename, destpath="/tmp"):
        """ """
        if not os.path.exists(destpath):
            os.makedirs(destpath)
        filepath = os.path.join(destpath, filename)
        shutil.copyfileobj(stream, open(filepath, 'wb'))
        current.logger.debug("File '%(filename)s' successfully copied to: %(destpath)s" % locals())
        return filepath

    @staticmethod
    def _splitxls(stream, filename, destpath="/tmp", destfilename=None, tabs=0, header_line=0):
        """ Split tabs of a single XLS file into multiple CSV files.
        Returns list of file paths.
        """

        if isinstance(tabs, int):
            tabs = (tabs,)

        def _getNewFilename(i):
            suffix = "_tab%s.csv" % i
            fn, __ext = os.path.splitext(destfilename or filename)
            return fn + suffix 

        def _loopOsheets():
            """ Loop over XLS file sheets """
            if filename.endswith("zip"):
                with zipfile.ZipFile(stream, "r") as cnt:
                    with xlrd.open_workbook(file_contents=cnt.read(cnt.namelist()[0])) as xlsSRC:
                        for sindex in (tabs or xrange(xlsSRC.nsheets)):
                            yield sindex, xlsSRC.sheet_by_index(sindex)
            else:
                with xlrd.open_workbook(file_contents=stream.read()) as xlsSRC:
                    for sindex in (tabs or xrange(xlsSRC.nsheets)):
                        yield sindex, xlsSRC.sheet_by_index(sindex)

        newcsvpaths = {}
        for sindex, sheet in _loopOsheets():
            newcsvpath = os.path.join(destpath, _getNewFilename(sindex))
            with open(newcsvpath, "w") as destcsv:
                mysreader = SheetReader(sheet, header_line=header_line)
                csvwriter = csv.DictWriter(destcsv, fieldnames=mysreader.header)
                csvwriter.writeheader()
                for r in mysreader():
                    csvwriter.writerow(r)
            newcsvpaths["tab%s" % sindex] = newcsvpath

        return newcsvpaths

    @staticmethod
    def _txt2csv(stream, filename, destpath="/tmp"):
        """ Convert stock text file into easier CSV file """
        newcsvpath = os.path.join(destpath, filename)
        with open(newcsvpath, "w") as destcsv:
            csvwriter = csv.DictWriter(destcsv, fieldnames=stock.header())
            csvwriter.writeheader()
            for r in stock.read(stream):
                csvwriter.writerow(r)
        return newcsvpath

    @classmethod
    def retrieve(cls, row, destpath=None):
        """ Retrieve from DB and save in temporary path
        Returns: {"tab<n>": <path>}
        """

        tab = db.archive

        dest_nfo = get_dest_parts(row.archname)
        filename, stream = tab.archive.retrieve(row.archive)

        destpath = destpath or get_dest_path(row.archname)
        destfilename = dest_nfo.get("name")

        out = {}

        # Some xls archives needs to be splitted into different csv from each tab;
        if row.archname in ("arch_cat", "arch_pri", "arch_apt",):
            return cls._splitxls(stream, filename, destpath, destfilename)
#         elif row.archname == "arch_apt":
#             return cls._splitxls(stream, filename, destpath, destfilename, header_line=2)

        # other needs to be simply converted from text to compatible CSV format;
        elif row.archname == "arch_avl":
            return {"tab0": cls._txt2csv(stream, destfilename or filename, destpath)}
        # all other are considered to be good CSV files.
        else:
            return {"tab0": cls._copy(stream, destfilename or filename, destpath)}

    @classmethod
    def build_updates(cls, clean=True):
        """ Create a complete data set merging informations from:
        last catalog, prices, refill and stock files available
        and save it to db as json string

        Returns record id.
        clean   @bool : whether keep the fs cleaned or not;
        """

        # 1. Extract last updated data from db
        repos = ("arch_cat", "arch_avl", "arch_pri", "arch_apt",)

        res = db(db.archive.archname.belongs(repos)).select()
        assert len(res)==len(repos), "Error!"

        actuals = res.group_by_value(db.archive.archname)
#         _actuals = db(db.archive.archname.belongs(repos))._select(db.archive.id)

        apaths = {an: cls.retrieve(rows[0]) for an,rows in actuals.iteritems()}

        # UID code columns for each archive type.
        UID = {
           "arch_cat": "Codice",
           "arch_pri": "Codice Articolo",
           "arch_avl": "Product SKU",
           "arch_apt": "Codice Articolo"
        }

        fields = {
            "arch_cat": None, # That means ALL columns!
            "arch_pri": ("Larghezza", "Profondità", "Altezza", "Peso",),
            "arch_apt": ("Prezzo Netto", "Inizio Promozione", "Fine Promozione",),
            "arch_avl": ("Stock Quantity",)
        }

        # Value adjustments
        adjustments = {
            # Weight adjustment (From gr to kg)
            "Peso": lambda v: v and ("%.3f" % (float(v)/1000)).replace(".", ","),
            "Prezzo Netto": lambda v: v and ("%.2f" % float(v)).replace(".", ",")
        }

        # Rows by uuid for each archive
        acnts = {}
        aflds = {}
        for an, nfo in apaths.iteritems():
            with open(nfo["tab0"]) as source:
                ardr = csv.DictReader(source)
                acnts[an] = {row[UID[an]]: row for row in ardr}
                if fields[an] is None:
                    aflds[an] = ardr.fieldnames
                else:
                    aflds[an] = fields[an]

        fieldnames = tuple(sum([list(aflds[k]) for k in repos], []))
        _buildRow = lambda puk,k: acnts["arch_avl"][puk].get(k, acnts["arch_pri"][puk].get(k, acnts["arch_cat"][puk].get(k, acnts["arch_apt"][puk].get(k))))

        data = {}
        for puk in sorted(set.intersection(*map(set, acnts.values()))):
            arow = {k: _buildRow(puk, k) for k in fieldnames}
            for k,f in adjustments.iteritems():
                arow[k] = f(arow[k])
            data[puk] = arow

        try:
            out = db.product_catalog.insert(
                pdata = data,
                fieldnames = fieldnames,
                checksums = sorted(set((r.checksum for r in res)))
            )
        except IntegrityError:
            db.rollback()
            return 0
        else:
            db.commit()
    
            if clean:
                for paths in [apaths,]:
                    for __an, nfo in paths.iteritems():
                        for __, filepath in nfo.iteritems():
                            keep_fs_clean(filepath)

        return out

    @staticmethod
    def compile_csv(n=10):
        """
        eventualmente si potrebbe distinguere se si hanno solo due versioni
        """
        outpath = get_dest_path("arch_cat")
        res = db(db.product_catalog.id>0).select(orderby=~db.product_catalog.id, limitby=(0, n,))
        puks = set(sum((row.pdata.keys() for row in res), []))

        data = res.last().pdata
        fieldnames = res.last().fieldnames

        out = {
            "all_product": os.path.join(outpath, "all_product.csv"),
            "updated_product": os.path.join(outpath, "updated_product.csv")
        }

#         allfile = os.path.join(outpath, "all_product.csv")
#         updfile = os.path.join(outpath, "updated_product.csv")
        with open(out["all_product"], "w") as allprd, \
            open(out["updated_product"], "w") as updprd:

            # Create and initialize output csv files
            allw = csv.DictWriter(allprd, fieldnames=fieldnames)
            updw = csv.DictWriter(updprd, fieldnames=fieldnames)
            allw.writeheader()
            updw.writeheader()

            for n,puk in enumerate(sorted(puks)):
                if any((puk==i for i in data.iterkeys())):
#                     import pdb; pdb.set_trace()
                    rowdict = {k.encode("utf8"): v.encode("utf8") for k,v in data[puk].iteritems()}
                    allw.writerow(rowdict)
                    if any(map(lambda c: not_equal(c[0], c[1]), ((res[n].pdata[puk], res[n+1].pdata[puk],) for n in range(len(res)-1)))):
                        updw.writerow(rowdict)

        return out