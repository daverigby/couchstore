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
#include <exception>
#include <atomic>

using namespace std;

enum VBucketState {
    VB_ACTIVE,
    VB_REPLICA,
    VB_UNMANAGED
};

class ProgramParameters {
public:

    enum DocType {
        BINARY_DOC, //only binary at the moment
        BINARY_DOC_COMPRESSED,
        JSON_DOC,
        JSON_DOC_COMPRESSED
    };

    static const bool reuse_couch_files_default = false;
    static const int vbc_default = 1024;
    static const int key_count_default = 2048;
    static const int keys_per_flush_default = 512;
    static const int doc_len_default = 256;
    static const DocType doc_type_default = BINARY_DOC;

    ProgramParameters()
        :
        reuse_couch_files(reuse_couch_files_default),
        vbc(vbc_default),
        key_count(key_count_default),
        keys_per_flush(keys_per_flush_default),
        doc_len(doc_len_default),
        doc_type(doc_type_default),
        vbuckets(vbc_default) {
        fill(vbuckets.begin(), vbuckets.end(), VB_UNMANAGED);
    }

    void load(int argc, char** argv) {

        while (1)
        {
            static struct option long_options[] =
            {
                {"reuse", no_argument, 0, 'r'},
                {"vbc", required_argument, 0, 'v'},
                {"keys", required_argument, 0, 'k'},
                {"keys_per_flush", required_argument, 0, 'f'},
                {"doc_len", required_argument, 0, 'd'},
                {0, 0, 0, 0}
            };
            /* getopt_long stores the option index here. */
            int option_index = 0;

            int c = getopt_long (argc, argv, "v:k:f:d:r", long_options, &option_index);

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

                case 'r': {
                    reuse_couch_files = true;
                    break;
                }

                default: {
                    usage(1);
                }
            }
        } // end of option parsing

        // Now are we managing all vbuckets, or a list?
        if (optind < argc)
        {
            while (optind < argc) {
                // a or r present?

                int i = atoi(argv[optind]);
                if(i < vbc) {
                    // a or r present?
                    VBucketState s = VB_ACTIVE;
                    for (size_t i; i < strlen(argv[optind]); i++) {
                        if (argv[optind][i] == 'a') {
                            s = VB_ACTIVE;
                        } else if (argv[optind][i] == 'r') {
                            s = VB_REPLICA;
                        }
                    }
                    vbuckets[i] = s;
                    cout << "Managing VB " << i;
                    if (s == VB_ACTIVE) {
                        cout << " active" << endl;
                    } else {
                        cout << " replica" << endl;
                    }

                    optind++;
                }
            }
        } else {
            for(int i = 0; i < vbc; i++) {
                vbuckets[i] = VB_ACTIVE;
            }
        }
    }

    // validate params
    bool validate() const {
        if (vbc <= 0) {
            cerr << "Error: vbc less than or equal to 0 - " << vbc << endl;
            return false;
        }
        return true;
    }

    int16_t getVbc() const {
        return vbc;
    }

    int getKeyCount() const {
        return key_count;
    }

    int getKeysPerFlush() const {
        return keys_per_flush;
    }

    int getDocLen() const {
        return doc_len;
    }

    bool getReuseCouchFiles() const {
        return reuse_couch_files;
    }

    string getDocTypeString() const {
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

    bool isVbucketManaged(int vb) const {
        if (vb > vbc) {
            return false;
        }
        return vbuckets[vb] != VB_UNMANAGED;
    }

    VBucketState getVBucketState(int vb) const {
        return vbuckets[vb];
    }

    void disableVbucket(int vb) {
        vbuckets[vb] =  VB_UNMANAGED;
    }

    static void usage(int exit_code) {
        cerr << endl;
        cerr << "couch_create <options> <vbucket list>" << endl;
        cerr << "options:" << endl;
        cerr << "    --reuse,-r: Reuse couch-files (any re-used file must have a vbstate document) (default " << reuse_couch_files_default << ")" << endl;
        cerr << "    --vbc, -v <integer>:  Number of vbuckets (default " << vbc_default << ")" << endl;
        cerr << "    --keys, -k <integer>:  Number of keys to create (default " << key_count_default << ")" << endl;
        cerr << "    --keys_per_flush, -f <integer>:  Number of keys per vbucket before committing to disk (default " << keys_per_flush_default << ")" << endl;
        cerr << "    --doc_len,-d <integer>:  Number of bytes for the document body (default " << doc_len_default << ")" << endl;
        cerr << endl << "vbucket list (optional space separated values):" << endl;
        cerr << "    Specify a list of vbuckets to manage and optionally the state. " <<
                "E.g. vbucket 1 can be specified as 1 (defaults to active if creating vbuckets) or 1a (for active) or 1r (for replica)" << endl << endl;

        cerr <<
            "Two modes of operation." << endl <<
            "    1) Re-use vbuckets (--reuse or -r) \"Automatic mode\"" << endl <<
            "    In this mode of operation the program will only write key/values into vbucket files it finds in the current directory." << endl <<
            "    Ideally the vbucket files are empty of documents, but must have a vbstate local doc." << endl <<
            "    The intent of this mode is for a cluster and bucket to be pre-created, but empty and then to simply "
            "populate the files found on each node without having to consider which are active/replica." << endl;

        cerr <<
            "    2) Create vbuckets" << endl <<
            "    In this mode of operation the program will create new vbucket files. The user must make the decision about what is active/replica" << endl << endl;

        cerr << "Examples: " << endl;
        cerr << "  Create 1024 active vbuckets containing 10,000, 256 byte binary documents." << endl;
        cerr << "    > ./couch_create -k 10000" << endl << endl;
        cerr << "  Iterate over 10,000 keys, but only generate vbuckets 0, 1, 2 and 3 with a mix of active/replica"<< endl;
        cerr << "    > ./couch_create -k 10000 0a 1r 2a 3r" << endl << endl;
        cerr << "  Iterate over 10,000 keys and re-use existing couch-files"<< endl;
        cerr << "    > ./couch_create -k 10000 -r" << endl << endl;

        exit(exit_code);
    }

private:

    bool reuse_couch_files;
    int16_t vbc;
    int key_count;
    int keys_per_flush;
    int doc_len;
    DocType doc_type;
    vector<VBucketState> vbuckets;
};

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

        size_t getSize() const {
            // Not safe to use sizeof(Meta) due to trailing padding
            return sizeof(cas) + sizeof(exptime) + sizeof(flags) + sizeof(flex_meta_code) + sizeof(flex_value);
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
        doc_info.rev_meta.size =  meta.getSize();
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

class VBucket {

public:
    class Exception1 : public exception {
        virtual const char* what() const throw() {return "Found an existing couch-file with vbstate and --reuse/-r is not set.";}
    } exception1;

    class Exception2 : public exception {
        virtual const char* what() const throw() {return "Didn't find valid couch-file (or found file with no vbstate) and --reuse/-r is set.";}
    } exception2;

    class Exception3 : public exception {
        virtual const char* what() const throw() {return "Error opening couch_file (check ulimit -n).";}
    } exception3;

    VBucket(char* filename,
            int vb,
            atomic_int& saved_counter,
            ProgramParameters& params_ref)
        :
        handle(NULL),
        next_free_doc(0),
        flush_threshold(params_ref.getKeysPerFlush()),
        docs(params_ref.getKeysPerFlush()),
        pending_documents(0),
        documents_saved(saved_counter),
        params(params_ref),
        vbid(vb) {
        couchstore_error_t err = couchstore_open_db(filename, COUCHSTORE_OPEN_FLAG_CREATE, &handle);
        if (err != COUCHSTORE_SUCCESS) {
            throw exception3;
        }

        if (read_vbstate()) {
            // A vbstate document exists.
            // Can only proceed if we're in reuse mode
            if (!params.getReuseCouchFiles()) {
                destroy();
                throw exception1;
            }
        } else {
            if (params.getReuseCouchFiles()) {
                destroy();
                throw exception2;
            }
            set_vbstate();
        }
    }

    ~VBucket() {
        save_docs();
        docs.clear();
        destroy();
    }

    //
    // Return true if the vbstate document is present.
    //
    bool read_vbstate() {
        LocalDoc* local_doc = nullptr;
        couchstore_error_t errCode =  couchstore_open_local_document(handle, "_local/vbstate", sizeof("_local/vbstate") - 1, &local_doc);
        if (local_doc) {
             couchstore_free_local_document(local_doc);
        }
        return errCode == COUCHSTORE_SUCCESS;
    }

    //
    // Set the vbstate of the document
    //
    void set_vbstate() {
        stringstream jsonState;
        string state_string = params.getVBucketState(vbid) == VB_ACTIVE ? "active" : "replica";
        jsonState << "{\"state\": \"" << state_string << "\""
                  << ",\"checkpoint_id\": \"0\""
                  << ",\"max_deleted_seqno\": \"0\""
                  << ",\"snap_start\": \"0\""
                  << ",\"snap_end\": \"0\""
                  << ",\"max_cas\": \"1\""
                  << ",\"drift_counter\": \"0\""
                  << "}";

        std::string vbstate_json = jsonState.str();
        vbstate.id.buf = (char *)"_local/vbstate";
        vbstate.id.size = sizeof("_local/vbstate") - 1;
        vbstate.json.buf = (char *)vbstate_json.c_str();
        vbstate.json.size = vbstate_json.size();
        vbstate.deleted = 0;

        couchstore_error_t errCode = couchstore_save_local_document(handle, &vbstate);
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
            documents_saved+=pending_documents;
            next_free_doc = 0;
            pending_documents = 0;
        }
    }

private:

    void destroy() {
        couchstore_close_file(handle);
        couchstore_free_db(handle);
    }

    Db* handle;
    int next_free_doc;
    int flush_threshold;
    vector< unique_ptr<Document> > docs;
    int pending_documents;
    atomic_int& documents_saved;
    LocalDoc vbstate;
    ProgramParameters& params;
    int vbid;
};

int main(int argc, char **argv) {


    ProgramParameters parameters;
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

    if (parameters.getReuseCouchFiles()) {
        cout << "Re-using any existing couch-files" << endl;
    } else {
        cout << "Creating new couch-files" << endl;
    }

    vector< unique_ptr<VBucket> > vb_handles(parameters.getVbc());

    atomic_int documents_saved(0);
    int jj = 0;

    for (int ii = 0; ii < parameters.getKeyCount(); ii++) {
        int key_len = snprintf(key, 64, "K%020" PRId64, key_value);
        int vbid = client_hash_crc32(reinterpret_cast<uint8_t*>(key), key_len) & (parameters.getVbc()-1);

        // Only if the vbucket is managed generate the doc
        if (parameters.isVbucketManaged(vbid)) {
            if (vb_handles[vbid] == nullptr) {
                char  filename[32];
                snprintf(filename, 32, "%d.couch.1", vbid);
                try {
                    vb_handles[vbid] = unique_ptr<VBucket>(new VBucket(filename,
                                                                       vbid,
                                                                       documents_saved,
                                                                       parameters));
                } catch(exception& e) {
                    cerr << "Not creating a VB handler for " << filename << " \"" << e.what() << "\"" << endl;
                    parameters.disableVbucket(vbid);
                }
            }

            // if there's now a handle, go for it
            if (vb_handles[vbid] != nullptr) {
                vb_handles[vbid]->add_doc(key, key_len, parameters.getDocLen());
            }

        }
        jj++;
        key_value++;
        if (jj >= ((parameters.getKeyCount() / 100) * percentage_report)) {
            percent += percentage_report;
            cout << percent << "%" << endl;
            jj = 0;
        }
    }

    vb_handles.clear();
    cout << "Saved " << documents_saved << " documents " << endl;

    return 0;
}
