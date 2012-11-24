(ns wrangler.core
  (:require [clojure-csv.core :as csv]
            [clojure.set :as set]))

(defn table [column-names data]
  {:column-names column-names
   :data (map (fn [row]
                (into {} (map vector column-names row)))
              data)})

(defn map-column [table mapper column]
  {:column-names (:column-names table)
   :data (map (fn [row]
                (update-in row [column]
                           #(mapper row %1)))
              (:data table))})

(defn- column-names-from-data [data]
       (into [] (reduce set/union #{} (map #(into #{} (keys %)) data))))

(defn combine-rows [table joins diffs keepers namer]
  (let [data (map (fn [[js j-rows]]
                (let [j-vals (map js joins)]
                  (apply merge
                         js
                         (map (fn [[ds d-rows]]
                                (assert (= 1 (count d-rows)))
                                (let [d-row (first d-rows)
                                      d-vals (map ds diffs)
                                      names (into {} (map #(vector %1 (namer j-vals d-vals %1)) keepers))]
                                  (into {} (map (fn [k]
                                                  [(names k) (d-row k)])
                                                keepers))))
                              (set/index j-rows diffs)))))
			  (set/index (:data table) joins))]
    {:column-names (column-names-from-data data)
     :data data}))

(defn reorder-columns [table column-names]
  {:column-names column-names
   :data (:data table)})

(defn column-values [table column]
  (map column
       (:data table)))

(defn csv [table]
  (csv/write-csv
    (let [column-names (:column-names table)]
      (concat [(map str column-names)]
              (map (fn [row]
                     (map #(str (row %)) column-names))
                   (:data table))))))

(comment

  (def pause-times-csv (slurp "/Users/schani/Work/clojure/wrangler/pausetimes.csv"))
(def pause-times (clojure-csv.core/parse-csv pause-times-csv))

(require '[wrangler.core :as wr])
(require '[clojure.string :as str])
(require '[clojure.set :as set])

(def table (wr/table [:benchmark :config :generation :median :max] pause-times))

(spit "/Users/schani/Work/clojure/wrangler/pausetimes-wrangled.csv"
(-> table
  (wr/map-column (fn [_ cfg]
                   (nth (str/split cfg #"/") 1))
                 :config)
  (wr/map-column (fn [_ cfg]
                   (nth (str/split cfg #"\.") 0))
                 :benchmark)
  (wr/map-column #(java.lang.Integer. %2) :generation)
  (wr/combine-rows [:benchmark :config]
                   [:generation]
                   [:median :max]
                   (fn [_ [gen] med-or-max]
                     ({[0 :median] :minor-median
                       [0 :max] :minor-max
                       [1 :median] :major-median
                       [1 :max] :major-max}
                       [gen med-or-max])))
  (wr/combine-rows [:benchmark]
                   [:config]
                   [:minor-median :minor-max :major-median :major-max]
                   (fn [_ [conf] mm]
                     (keyword (str (name conf) "-" (name mm)))))
  (wr/reorder-columns (concat [:benchmark]
                              (for [c ["nonconc" "conc"]
                                    l ["nonlazy" "lazy"]
                                    g ["minor" "major"]
                                    m ["median" "max"]]
                                (keyword (str "sgen-" l "-" c "-" g "-" m)))))
  (wr/csv)))

  )
