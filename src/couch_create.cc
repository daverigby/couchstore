#include "libcouchstore/couch_db.h"
#include "crc32.h"
#include <string>
#include <sstream>
#include <vector>
#include <iostream>
#include <stdint.h>
#include <cstring>
#include <memory>
#include <stddef.h>
#include <inttypes.h>
#include <getopt.h>

using namespace std;

const size_t COUCHSTORE_METADATA_SIZE(2 * sizeof(uint32_t) + sizeof(uint64_t) +
                                      1 + 1);

class Document {
    class Meta {
    public:
        Meta(uint64_t c, uint32_t e, uint32_t f) : cas(c), exptime(e), flags(f), flex_meta_code(0x01), flex_value(0x0) {
        }

        void setCas(uint64_t cas) {
            this->cas = cas;
        }

        void setExptime(uint32_t exptime) {
            this->exptime = exptime;
        }

        void setFlags(uint32_t flags) {
            this->flags = flags;
        }

    public:
        uint64_t cas;
        uint32_t exptime;
        uint32_t flags;
        uint8_t flex_meta_code;
        uint8_t flex_value;
    };

public:
    Document(const char* k, int klen, int dlen)
    : meta(1, 0, 0),
      key_len(klen),
      key(NULL),
      data_len(dlen),
      data(NULL) {
        key = new char[klen];
        data = new char[dlen];
        set_doc(k, klen, dlen);
        doc.id.buf = key;
        doc.id.size = klen;
        doc.data.buf = data;
        doc.data.size = dlen;
        doc_info.id = doc.id;
        doc_info.size = doc.data.size;
        doc_info.db_seq = 0;//db_seq;
        doc_info.rev_seq = 1;// ++db_seq;
        doc_info.content_meta = COUCH_DOC_NON_JSON_MODE;
        doc_info.rev_meta.buf = reinterpret_cast<char*>(&meta);
        doc_info.rev_meta.size =  COUCHSTORE_METADATA_SIZE;
        doc_info.deleted = 0;
    }

    ~Document() {
        delete [] key;
        delete [] data;
    }

    void set_doc(const char* k, int klen, int dlen) {
        if (key && (klen > key_len)) {
            delete key;
            key = new char[klen];
            doc.id.buf = key;
            doc.id.size = klen;
            doc_info.id = doc.id;
        }
        if (data && (dlen > data_len)) {
            delete data;
            data = new char[dlen];
            doc.data.buf = data;
            doc.data.size = dlen;
        }
        // Generate ascii data and copy-in key
        memcpy(key, k, klen);
        char data_value = 0;
        for (int data_index = 0; data_index < dlen; data_index++) {
            data[data_index] = data_value + '0';
            data_value = (data_value + 1) % ('Z' - '0');
        }
    }

    Doc* getDoc() {
        return &doc;
    }

    DocInfo* getDocInfo() {
        return &doc_info;
    }
private:
    Doc doc;
    DocInfo doc_info;
    Meta meta;

    int key_len;
    char* key;
    int data_len;
    char* data;

    static uint64_t db_seq;
};

uint64_t Document::db_seq = 0;

class DbStuff {
public:
    DbStuff(char* filename, int flush, int* saved_counter)
    : handle(NULL), next_free_doc(0), flush_threshold(flush), docs(flush), pending_documents(0), documents_saved(saved_counter) {
        couchstore_error_t err = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_CREATE, &handle);
        if (err != COUCHSTORE_SUCCESS) {
                cerr << "Error with couchstore_open_db " <<  couchstore_strerror(err) << endl;
        }
        add_special_vbstate_doc();
    }

    ~DbStuff() {
        save_docs();
        docs.clear();
        couchstore_close_file(handle);
        couchstore_free_db(handle);
    }

    void add_special_vbstate_doc() {
        stringstream jsonState;

        jsonState << "{\"state\": \"active\""
                  << ",\"checkpoint_id\": \"0\""
                  << ",\"max_deleted_seqno\": \"0\""
                  << ",\"snap_start\": \"0\""
                  << ",\"snap_end\": \"0\""
                  << ",\"max_cas\": \"1\""
                  << ",\"drift_counter\": \"0\""
                  << "}";

        LocalDoc lDoc;
        lDoc.id.buf = (char *)"_local/vbstate";
        lDoc.id.size = sizeof("_local/vbstate") - 1;
        std::string state = jsonState.str();
        lDoc.json.buf = (char *)state.c_str();
        lDoc.json.size = state.size();
        lDoc.deleted = 0;

        couchstore_error_t errCode = couchstore_save_local_document(handle, &lDoc);
        if (errCode != COUCHSTORE_SUCCESS) {
            cerr << "Warning: couchstore_save_local_document failed error="
                 << couchstore_strerror(errCode) << endl;
        }
    }

    void add_doc(char* k, int klen, int dlen) {
        if (docs[next_free_doc] == nullptr) {
            docs[next_free_doc] = unique_ptr<Document>(new Document(k, klen, dlen));
        }

        docs[next_free_doc]->set_doc(k, klen, dlen);
        pending_documents++;

        if (pending_documents == flush_threshold) {
            save_docs();
        } else {
            next_free_doc++;
        }
    }

    void save_docs() {
        if(pending_documents) {
            vector<Doc*> doc_array(pending_documents);
            vector<DocInfo*> doc_info_array(pending_documents);

            for(int i = 0; i < pending_documents; i++) {
                doc_array[i] = docs[i]->getDoc();
                doc_info_array[i] = docs[i]->getDocInfo();
            }

            couchstore_save_documents(handle, doc_array.data(), doc_info_array.data(), pending_documents, 0);
            couchstore_commit(handle);
            *documents_saved+=pending_documents;
            next_free_doc = 0;
            pending_documents = 0;
        }
    }

private:
    Db* handle;
    int next_free_doc;
    int flush_threshold;
    vector< unique_ptr<Document> > docs;
    int pending_documents;
    int * documents_saved;
};

void usage() {
    cerr << endl;
    cerr << "couch_create <options> <vbuckets>" << endl;
    cerr << "    --vbc, -v <integer>  number of vbuckets (default 1024)" << endl;
    cerr << "    --keys, -k <integer>  number of keys to create (default 1024)" << endl;
    cerr << "    --keys_per_flush, -f <integer>  number of keys per vbucket before committing to disk (default 50)" << endl;
    cerr << "    --doc_len,-d <integer>  number of bytes for the doc_body (default 256)" << endl << endl;
    cerr << "Finally, optionally specify a list of vbuckets to manage. E.g. only vbuckets 0, 1, 2 and 3" << endl;
    cerr << "  ./couch_create -k 20000 0 1 2 3"<< endl;
    cerr << "NB: this won't create 20000 documents, only those from the 20000 which match the vbuckets." << endl;
    exit(1);
}

class ProgramParameters {
public:

    enum DocType {
        BINARY_DOC, //only binary at the moment
        BINARY_DOC_COMPRESSED,
        JSON_DOC,
        JSON_DOC_COMPRESSED
    };

    ProgramParameters(int vbc_init,
                      int key_count_init,
                      int keys_per_flush_init,
                      int doc_len_init,
                      DocType doc_type_init)
        : vbc(vbc_init),
          key_count(key_count_init),
          keys_per_flush(keys_per_flush_init),
          doc_len(doc_len_init),
          doc_type(doc_type_init),
          vbuckets(vbc) {
    }

    void load(int argc, char** argv) {

        while (1)
        {
            static struct option long_options[] =
            {
                {"vbc", required_argument, 0, 'v'},
                {"keys", required_argument, 0, 'k'},
                {"keys_per_flush", required_argument, 0, 'f'},
                {"doc_len", required_argument, 0, 'd'},
                {0, 0, 0, 0}
            };
            /* getopt_long stores the option index here. */
            int option_index = 0;

            int c = getopt_long (argc, argv, "v:k:f:d:", long_options, &option_index);

            /* Detect the end of the options. */
            if (c == -1) {
                break;
            }

            switch (c)
            {
                case 'v': {
                    vbc = static_cast<int16_t>(atoi(optarg));
                    vbuckets.resize(vbc);
                    break;
                }

                case 'k': {
                    key_count = atoi(optarg);
                    break;
                }

                case 'f': {
                    keys_per_flush = atoi(optarg);
                    break;
                }

                case 'd': {
                    doc_len = atoi(optarg);
                    break;
                }

                default: {
                    usage();
                }
            }
        } // end of option parsing

        // Now any vbuckets for limiting?
        if (optind < argc)
        {
            while (optind < argc) {
                int i = atoi(argv[optind++]);
                if(i < vbc) {
                    vbuckets[i] = true;
                    cout << "Managing VB " << i << endl;
                }
            }
        } else {
            for(int i = 0; i < vbc; i++) {
                vbuckets[i] = true;
            }
        }
    }

    // validate params
    bool validate() {
        if (vbc <= 0) {
            cerr << "Error: vbc less than or equal to 0 - " << vbc << endl;
            return false;
        }
        return true;
    }

    int16_t getVbc() {
        return vbc;
    }

    int getKeyCount() {
        return key_count;
    }

    int getKeysPerFlush() {
        return keys_per_flush;
    }

    int getDocLen() {
        return doc_len;
    }

    string getDocTypeString() {
        switch(doc_type) {
            case BINARY_DOC:
            {
                return string("binary");
                break;
            }
            case BINARY_DOC_COMPRESSED: {
                return string("binary compressed");
                break;
            }
            case JSON_DOC: {
                return string("JSON");
                break;
            }
            case JSON_DOC_COMPRESSED: {
                return string("JSON compressed");
                break;
            }
        }
        return string("getDocTypeString failure");
    }

    bool isVbucketManaged(int vb) {
        if (vb > vbc) {
            return false;
        }
        return vbuckets[vb];
    }

private:
    int16_t vbc;
    int key_count;
    int keys_per_flush;
    int doc_len;
    DocType doc_type;
    vector<bool> vbuckets;
};

int main(int argc, char **argv) {


    ProgramParameters parameters(1024, 2048, 1024, 256, ProgramParameters::BINARY_DOC);
    parameters.load(argc, argv);
    if (!parameters.validate()) {
        return 1;
    }

    uint64_t key_value = 0;
    char key[64];
    int percent = 0;
    const int percentage_report = 5;


    cout << "Generating " << parameters.getKeyCount() << " keys" << endl;
    if (parameters.getKeyCount() > 1) {
        snprintf(key, 64, "K%020" PRId64, (uint64_t)parameters.getKeyCount() - 1);
        cout << "Key pattern is K00000000000000000000 to " << key << endl;
    } else {
        cout << "Key pattern is K00000000000000000000" << endl;
    }

    cout << "vbucket count set to " << parameters.getVbc() << endl;
    cout << "keys per flush set to " << parameters.getKeysPerFlush() << endl;
    cout << "Document type is " << parameters.getDocLen() << " bytes " << parameters.getDocTypeString() << endl;

    vector< unique_ptr<DbStuff> > db_handles(parameters.getVbc());

    int documents_saved = 0;
    int jj = 0;

    for (int ii = 0; ii < parameters.getKeyCount(); ii++) {
        int key_len = snprintf(key, 64, "K%020" PRId64, key_value);
        int vbid = client_hash_crc32(reinterpret_cast<uint8_t*>(key), key_len) & (parameters.getVbc()-1);

        // Only if the vbucket is managed generate the doc
        if (parameters.isVbucketManaged(vbid)) {
            if (db_handles[vbid] == nullptr) {
                char  filename[32];
                snprintf(filename, 32, "%d.couch.1", vbid);
                db_handles[vbid] = unique_ptr<DbStuff>(new DbStuff(filename, parameters.getKeysPerFlush(), &documents_saved));
            }
            db_handles[vbid]->add_doc(key, key_len, parameters.getDocLen());
        }
        jj++;
        key_value++;
        if (jj >= ((parameters.getKeyCount() / 100) * percentage_report)) {
            percent += percentage_report;
            cout << percent << "%" << endl;
            jj = 0;
        }
    }

    db_handles.clear();
    cout << "Saved " << documents_saved << " documents " << endl;

    return 0;
}
