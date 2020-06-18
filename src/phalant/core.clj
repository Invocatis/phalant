(ns phalant.core
  (:require
    [phalant.util :refer :all]
    [clojure.java.jdbc :as jdbc]
    [clojure.edn :as edn]
    [cheshire.core :as json]
    [taoensso.nippy :as nippy]
    [postgre-types.json :refer [add-json-type add-jsonb-type]])
  (:import
    org.postgresql.util.PGobject)
  (:refer-clojure :exclude [replace remove]))

(def db {:dbtype "postgresql"
         :dbname "asdf"
         :host "localhost"
         :user "ljc237-admin"})

(def schema {[:asdf] "test1"})
(def schema nil)

(declare -query)

(defn complex?
  [any]
  (or (set? any) (map? any) (vector? any)))

(defn decode
  [v]
  (when v
    (nippy/thaw v)))

(defn encode
  [v]
  (nippy/freeze v))

(defn encode-table
  [t]
  (cond
    (string? t) t
    :else (format "%s" (pr-str t))))

(defn decode
  [v]
  (cond
    (string? v) (read-string v)
    :else v))

(defn encode
  [any]
  (cond
    (string? any) any
    (number? any) any
    :else (pr-str any)))

(defn json-encode
  [value]
  (json/generate-string value {:key-fn pr-str}))

(defn json-decode
  [value]
  (json/parse-string value edn/read-string))

(add-jsonb-type json-encode json-decode)

(defn value->row
  [path value]
  (let [value (if (complex? value) (empty value) value)]
    {:id (into [] (map encode path))
     :metadata (-meta value)
     :value (encode value)}))

(defn row->value
  [{:keys [metadata value]}]
  (-with-meta value metadata))

(defn decode-row
  [row]
  (-> row
    (update :id #(vec (map decode %)))
    (update :value decode)))

(defmacro transaction
  [binding & body]
  `(jdbc/with-db-connection ~binding ~@body))

(defn create-tree-node-table
  [db name]
  (jdbc/execute! db
    (jdbc/create-table-ddl name
      [[:id :jsonb "primary key"]
       [:details :jsonb "default '{}'"]
       [:metadata :jsonb "default '{}'"]
       [:value :text]]
      {:conditional? true
       :entities (jdbc/quoted [\" \"])})))

(defn synthesize
  [results]
  (if (empty? results)
    nil
    (loop [results results
           m {}]
      (if (empty? results)
        m
        (let [{:keys [id value metadata]} (decode-row (first results))]
          (if (empty? id)
            (recur (rest results) (-with-meta value metadata))
            (recur (rest results) (conj-in m id (-with-meta value metadata)))))))))

(defn query-string
  [table path]
  [(format "SELECT *
            FROM \"%s\"
            WHERE jsonb_array_starts_with(id, ?)
              AND ? <= jsonb_array_length(id)
            ORDER BY id;"
           table)
   (->> path (map encode) vec)
   (count path)])

(defn get-value-string
   [table path]
   (format "SELECT * FROM %s WHERE id = '[%s]'"
     table
     (->> path (map encode) (interpose ",") (apply str))))

(defn -query
  [db table path]
  (synthesize
    (map
      (fn [entry] (update entry :id #(drop (count path) %)))
      (jdbc/query db
        (query-string table path)
        {:entities (jdbc/quoted [\" \"])}))))

(defn -get-value
  [db table path]
  (decode-row
    (first
      (jdbc/query db
        [(format "SELECT * FROM \"%s\" WHERE id = ?"
           (encode-table table)) (vec (map encode path))]))))

(defn insert-fields
  [fields]
  (->> fields (map name)))

(defn update-or-insert!
  "Updates columns or inserts a new row in the specified table"
  [db table row where-clause]
  (jdbc/with-db-transaction [t-con db]
    (let [result (jdbc/update! t-con table row where-clause
                   {:entities (jdbc/quoted [\" \"])})]
      (if (zero? (first result))
        (jdbc/insert! t-con table row {:entities (jdbc/quoted [\" \"])})
        result))))

(defn entry-set
  [any]
  (cond
    (map? any) (seq any)
    (set? any) (let [vs (seq any)] (map vector vs vs))
    (vector? any) (map vector (range 0 (count any)) any)))

(defn -remove
  [db table path]
  (jdbc/delete! db
    (str \" (encode-table table) \")
    ["jsonb_array_starts_with(id,?) " (vec (map encode path))]))

(defn remove
  [db table path]
  (let [table (encode-table table)]
    (jdbc/with-db-transaction [tx db]
      (-remove tx table path))))

(defn coerce-path
  [path current value]
  (condp =
    {} path
    #{} (conj value path)))

(declare -insert)

(defn -insert-on-set
  [db table path v]
  (if (empty? path)
    (-insert db table [v] v)
    (let [current (-query db table (butlast path))]
      (if (contains? current (last path))
        (let [result (-insert db table (butlast path) v)]
          (-remove db table path)
          result)
        :asdf))))

(defn -insert
  [db table path v]
  (if (contains? schema (rest path))
    (recur db (get schema path) path v)
    (let [{:keys [id] :as row} (value->row path v)
          current (-query db table path)]
      (cond
        (set? current)
        (-insert-on-set db table path v)
        (and (vector? current) (empty? (rest path)))
        (let [count (count current)]
          (recur db table (conj path count) v))
        (complex? v)
        (let [entries (entry-set v)]
          (when (not= current v)
            (-remove db table path))
          (update-or-insert! db table
                             row
                             ["id = ?" id])
          (doall (map
                  (fn [[k v]]
                    (-insert db table (conj (vec path) k) v))
                  entries)))
        :else
        (update-or-insert! db
                           table
                           (value->row path v)
                           ["id = ?" id])))))

(defn insert
  [db table path v]
  (let [table (encode-table table)]
    (jdbc/with-db-transaction [tx db]
      (create-tree-node-table tx table)
      (-insert tx table path v))))

(defn -replace
  [db table path v]
  (-remove db table path)
  (-insert db table path v))

(defn replace
  [db table path v]
  (let [table (encode-table table)]
    (jdbc/with-db-transaction [tx db]
      (create-tree-node-table tx table)
      (-replace tx table path v))))

(defn -update
  [db table path f]
  (let [current (-query db table path)
        new (apply f [current])]
    (-replace db table path new)))

(defn update
  [db table path f]
  (let [table (encode-table table)]
    (jdbc/with-db-transaction [tx db]
      (create-tree-node-table tx table)
      (-update db table path f))))

(defn -get-children-string
  [table path]
  [(format "SELECT * FROM \"%s\" WHERE jsonb_array_starts_with(id, ?) AND jsonb_array_length(id) = %s"
           table
           (inc (count path)))
   (vec (map encode path))])

(defn -get-children
  [db table path]
  (jdbc/query db
    (-get-children-string table path)))

(defn -drop
  [db table path]
  (let [current (-get-value db table path)]
    (if (complex? current)
      (-remove db table path)
      (let [[s args] (-get-children-string table path)
            s (str s " ORDER BY id DESC LIMIT 1")
            {:keys [id]} (first (jdbc/query db [s args]))]
        (-remove db table id)))))

(defn drop
  [db table path & [v]]
  (let [table (encode-table table)]
    (jdbc/with-db-transaction [tx db]
      (create-tree-node-table tx table)
      (-drop tx table path v))))

(defn -get
  [db table path]
  (synthesize
    (map
      #(update % :id (fn [entry] (clojure.core/drop (count path) entry)))
      (cons
        (-get-value db table path)
        (-get-children db table path)))))

(defn query
  [db table path]
  (let [table (encode-table table)]
    (jdbc/with-db-transaction [tx db]
      (-query tx table path))))
