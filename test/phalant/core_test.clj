(ns phalant.core-test
  (:require [clojure.test :refer :all]
            [phalant.core :refer :all]))


; (def db {:dbtype "postgresql"
;          :dbname "asdf"
;          :host "localhost"
;          :user "ljc237-admin"})

(deftest test1
 (let [value ^{1 1, 2 2} {1 {2 {3 [4]}}}]
   (testing
     (insert db :test1 [] value)
     (is (= (query db :test1 []) value))
     (is (= (meta (query db :test1 [])) (meta value)))
     (is (= (query db :test1 [1 2]) (get-in value [1 2])))
     (insert db :test1 [1 2 3] 5)
     (is (= (query db :test1 [1 2 3]) [4 5]))
     (drop db :test1 [1 2 3])
     (is (= (query db :test1 [1 2 3]) [4])))))

(deftest replace-test
  (testing
    (replace db :test2 [] {:x 1 :y 2})
    (is (= {:x 1 :y 2} (query db :test2 [])))
    (replace db :test2 [] {:y 3 :z 4})
    (is (= {:x 1 :y 3 :z 4} (query db :test2 [])))))
