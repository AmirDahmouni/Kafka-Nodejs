import avro from 'avsc';

export default avro.Type.forSchema({
  type: 'record',
  fields: [
    {
      name: 'category',
      type: { type: 'enum', symbols: ['CAT', 'DOG',"MONKEY","LIONESS","SHEEP"] }
    },
    {
      name: 'noise',
      type: 'string',
    }
  ]
});
