package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Join sql test helper
 */
public class JoinSqlTestHelper {

    /** */
    private static final int ORG_COUNT = 100;

    /** */
    private static final int PERSON_PER_ORG_COUNT = 10;
    /** */
    public static final String JOIN_SQL = "select * from Person, \"org\".Organization as org " +
        "where Person.orgId = org.id " +
        "and lower(org.name) = lower(?)";

    /**
     * Populate organization cache with test data
     * @param cache @{IgniteCache}
     */
    public static void populateDataIntoOrg(IgniteCache<String, Organization> cache) {
        for (int i = 0; i < ORG_COUNT; i++) {
            Organization org = new Organization();
            org.setId("org" + i);
            org.setName("Organization #" + i);
            cache.put(org.getId(), org);
        }
    }

    /**
     * Populate person cache with test data
     * @param cache @{IgniteCache}
     */
    public static void populateDataIntoPerson(IgniteCache<String, Person> cache) {
        int personId = 0;
        for (int i = 0; i < ORG_COUNT; i++) {
            Organization org = new Organization();
            org.setId("org" + i);
            org.setName("Organization #" + i);

            for (int j = 0; j < PERSON_PER_ORG_COUNT; j++) {
                Person prsn = new Person();
                prsn.setId("pers" + personId);
                prsn.setOrgId(org.getId());
                prsn.setName("Person name #" + personId);

                cache.put(prsn.getId(), prsn);

                personId++;
            }
        }
    }

    /**
     *
     */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String orgId;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        public String getId() {
            return id;
        }

        /** */
        public void setId(String id) {
            this.id = id;
        }

        /** */
        public String getOrgId() {
            return orgId;
        }

        /** */
        public void setOrgId(String orgId) {
            this.orgId = orgId;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }
    }

    /**
     *
     */
    public static class Organization {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        public void setId(String id) {
            this.id = id;
        }

        /** */
        public String getId() {
            return id;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }
    }

}
