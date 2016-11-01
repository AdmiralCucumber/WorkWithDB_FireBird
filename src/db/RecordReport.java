package db;

import java.util.Objects;

public class RecordReport implements Comparable<Object> {

    private String[] record;
    private int numFildSort;

    public RecordReport(int cntRec, int numFildSort) {
        record = new String[cntRec];
        this.numFildSort = numFildSort;
    }

    public RecordReport(int cntRec) {
        record = new String[cntRec];
        this.numFildSort = -1;
    }

    public void putItemField(int i, String iStr) {
        record[i - 1] = iStr;
    }

    public int getSize() {
        return record.length;
    }

    public String getField(int iField) {
        if (record[iField] == null) return "";
        return record[iField];
    }

    private String getSortFieldLowerCase() {
        if (numFildSort == -1) return "";
        if (record[numFildSort] == null) return "";
        return record[numFildSort].toLowerCase();
    }

    public int hashCode() {
        if (numFildSort == -1) return -1;
        return record[numFildSort].hashCode();
    }

    public boolean equals(Object o) {
        if (numFildSort == -1) return false;
        if (o instanceof RecordReport) {
            RecordReport other = (RecordReport) o;
            return (Objects.equals(record[numFildSort], other.record[numFildSort]));
        }
        return false;
    }

    public String toStrBySeparatorAndExtracor(String strSeparator, String strExtracor) {
        String iStr = "";
        boolean flFirst = false;
        for (String aRecord : record) {
            if (flFirst) iStr += strSeparator;
            else flFirst = true;
            if (Objects.equals(aRecord, "")) aRecord = "---";
            iStr += strExtracor + aRecord + strExtracor;
        }
        return iStr;
    }

    @Override
    public String toString() {
        return toStrBySeparatorAndExtracor(" | ", "");
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof RecordReport) {
            String iStr = o.toString();
            return getSortFieldLowerCase().compareTo(iStr);
        }
        return 0;
    }

    public boolean hasFieldData(String strFieldData) {
        for (String aRecord : record) {
            if (Objects.equals(aRecord, strFieldData)) {
                return true;
            }
        }
        return false;
    }
}
