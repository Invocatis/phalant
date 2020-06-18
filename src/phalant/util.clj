(ns phalant.util)

(defn metable?
  [any]
  (instance? clojure.lang.IMeta any))

(defn -meta
  [any]
  (if (metable? any)
    (meta any)
    {}))

(defn -with-meta
  [any meta]
  (if (metable? any)
    (with-meta any meta)
    any))


(defn padr
  [l n & [val]]
  (into (empty l) (concat l (take (- n (count l)) (repeat val)))))

(defn conj-in
  [target path value]
  (if (empty? path)
    value
    (let [new (conj-in (get target (first path)) (rest path) value)]
      (cond
        (or (nil? target) (map? target))
        (assoc target (first path) new)
        (vector? target)
        (assoc (padr target (first path)) (first path) new)
        (set? target)
        (conj target new)))))
